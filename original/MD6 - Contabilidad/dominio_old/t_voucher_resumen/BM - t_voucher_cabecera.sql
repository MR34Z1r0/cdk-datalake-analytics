use [odsdwaje]
go
/****** object:  storedprocedure [dbo].[usp_tran_t_voucher_cabecera]    script date: 3/3/2025 6:34:23 pm ******/
set ansi_nulls on
go
set quoted_identifier on
go
alter procedure [dbo].[usp_tran_t_voucher_cabecera] @fechaejecucion varchar(20) = '' 
/* 
sistema        : ajegroup_ssis_ecuador
nombre         : usp_tran_t_voucher_cabecera
descripción    : control historico de la tabla t_voucher_cabecera
autor          : daphne orihuela - cosapi
fecha creación : 12-05-2014
-------------------------------------------------------------------------------------
fecha       descripción                                              usuario
------------------------------------------------------------------------------------- 
*/ 
as
begin 
	declare @nombretabla varchar(50)
    declare @valorvalidacion int 
    set @nombretabla = 't_voucher_cabecera' 
	 
	exec stgdwaje.dbo.conf_validar_tabla @nombretabla, @fechaejecucion, @valorvalidacion output 
	begin try 
	if @valorvalidacion = 1
	begin 
		select compania, sucursal, ejercicio, periodoctb, asiento, comprobant, centrocost, moneda, fecha, nota1, nota2,
		totdebe, tothaber, stsvoucher, feccontab, usucrea, feccrea, horcrea, ultusumod, fecultmod, horultimod,
		bd_origen, fecha_proceso
		into #tvouch1f 
		from stgdwaje.dbo.stg_tvouch1f t  
		 
		if @@rowcount = 0
		begin
			insert into stgdwaje.dbo.rej_tvouch1f(id_error, fecha_proceso, desc_error) 
			values (3, @fechaejecucion, 'stage sin data')
			execute stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 0
		end
		else
		begin
			 
			insert into stgdwaje.dbo.rej_tvouch1f(id_error, fecha_proceso, bd_origen, desc_error)
			select 3, @fechaejecucion, a.descripcion, 'bd: ' + a.descripcion + ' vacia'
			from stgdwaje..conf_tabla_dominio a where id_grupo = 2 and id_orden > 0 and valor2 ='activo' 
			and not exists (select 1 from #tvouch1f b where a.descripcion = b.bd_origen) 
			 
			delete t 
			from #tvouch1f t 
			where exists(select 1 from dbo.t_voucher_eliminados te where t.compania = te.cod_compania 
			and t.sucursal = te.cod_sucursal and t.ejercicio = te.cod_ejercicio and t.periodoctb = te.cod_periodo 
			and t.asiento = te.asiento and t.comprobant = te.nro_comprobante)
			 
			update t1 
			set t1.moneda = (select moneda_mn from dbo.m_compania m where m.cod_compania = t1.compania)
			from #tvouch1f t1 
			where not exists (select 1 from dbo.m_moneda m where m.cod_compania = t1.compania 
								and m.cod_moneda = t1.moneda) 
			
			update t1 
			set t1.feccontab = 0
			from #tvouch1f t1 
			where not exists (select 1 from dbo.m_fecha m where m.cod_fecha = t1.feccontab)
			
			update t1 
			set t1.sucursal = '00'
			from #tvouch1f t1 
			where not exists (select 1 from dbo.m_sucursal m where m.cod_compania = t1.compania and m.cod_sucursal = t1.sucursal)
			 
			select compania, sucursal, ejercicio, periodoctb, asiento, comprobant, centrocost, moneda, fecha, nota1, nota2,
			totdebe, tothaber, stsvoucher, feccontab, usucrea, feccrea, horcrea, ultusumod, fecultmod, horultimod,
			bd_origen, fecha_proceso,
			id_error = 5,
			desc_error= case when not exists (select 1 from dbo.m_periodo m where m.cod_compania = a.compania 
								and m.cod_ejercicio = a.ejercicio and m.cod_periodo = a.periodoctb) 
									then 'no existe foreign para m_periodo'
							when not exists (select 1 from dbo.m_sucursal m where m.cod_compania = a.compania 
								and m.cod_sucursal = a.sucursal) 
									then 'no existe foreign para m_sucursal'	 
							when not exists (select 1 from dbo.m_moneda m where m.cod_compania = a.compania 
												and m.cod_moneda = a.moneda)
									 then 'no existe foreign para m_moneda' 
							when (feccontab <> 0 and not exists (select 1 from dbo.m_fecha m where m.cod_fecha = a.feccontab))
									 then 'no existe foreign para m_fecha - fecha_contable'																		 
							end		 				
			into #tvouch1f_inconsistencia
			from #tvouch1f a
			where not exists (select 1 from dbo.m_periodo m where m.cod_compania = a.compania
							and m.cod_ejercicio = a.ejercicio and m.cod_periodo = a.periodoctb) 
				or not exists (select 1 from dbo.m_sucursal m where m.cod_compania = a.compania 
								and m.cod_sucursal = a.sucursal)
				or not exists (select 1 from dbo.m_moneda m where m.cod_compania = a.compania 
								and m.cod_moneda = a.moneda) 
				or (feccontab <> 0 and not exists (select 1 from dbo.m_fecha m where m.cod_fecha = a.feccontab)) 				
		 							
			insert into stgdwaje.dbo.rej_tvouch1f(compania, sucursal, ejercicio, periodoctb, asiento, comprobant,
			centrocost, moneda, fecha, nota1, nota2, totdebe, tothaber, stsvoucher, feccontab, usucrea, feccrea, 
			horcrea, ultusumod, fecultmod, horultimod, bd_origen, fecha_proceso, id_error, desc_error)			
			select compania, sucursal, ejercicio, periodoctb, asiento, comprobant,
			centrocost, moneda, fecha, nota1, nota2, totdebe, tothaber, stsvoucher, feccontab, usucrea, feccrea, 
			horcrea, ultusumod, fecultmod, horultimod, bd_origen, fecha_proceso, id_error, desc_error
			from #tvouch1f_inconsistencia
			 
			delete t 
			from #tvouch1f t
			where exists (select 1 from #tvouch1f_inconsistencia a 
				where t.compania = a.compania and t.sucursal = a.sucursal and t.ejercicio = a.ejercicio 
				and t.periodoctb = a.periodoctb and t.asiento = a.asiento and t.comprobant = a.comprobant)		
 
			select * 
			into #tvouch1f_duplicado 
			from #tvouch1f a
			where exists (select 1 from #tvouch1f b
						 where a.compania = b.compania and a.sucursal = b.sucursal and a.ejercicio = b.ejercicio 
						 and a.periodoctb = b.periodoctb and a.asiento = b.asiento and a.comprobant = b.comprobant
						 group by compania, sucursal, ejercicio, periodoctb, asiento, comprobant having count(*) > 1)
				
			insert into stgdwaje.dbo.rej_tvouch1f(compania, sucursal, ejercicio, periodoctb, asiento, comprobant,
			centrocost, moneda, fecha, nota1, nota2, totdebe, tothaber, stsvoucher, feccontab, usucrea, feccrea, 
			horcrea, ultusumod, fecultmod, horultimod, bd_origen, fecha_proceso, id_error, desc_error)			
			select compania, sucursal, ejercicio, periodoctb, asiento, comprobant,
			centrocost, moneda, fecha, nota1, nota2, totdebe, tothaber, stsvoucher, feccontab, usucrea, feccrea, 
			horcrea, ultusumod, fecultmod, horultimod, bd_origen, fecha_proceso, 2, 'clave duplicada'
			from #tvouch1f_duplicado
				
			delete t 
			from #tvouch1f t
			where exists (select 1 from #tvouch1f_duplicado a 
				where a.compania = t.compania and a.sucursal = t.sucursal and a.ejercicio = t.ejercicio 
				and a.periodoctb = t.periodoctb and a.asiento = t.asiento and a.comprobant = t.comprobant)
				   
			delete t 
			from dbo.t_voucher_cabecera t
			where exists (select 1 from #tvouch1f a 
				where a.compania = t.cod_compania and a.ejercicio = t.cod_ejercicio and a.periodoctb = t.cod_periodo)
						 		 
			insert into dbo.t_voucher_cabecera (cod_compania, cod_sucursal, cod_ejercicio, cod_periodo, asiento, nro_comprobante,
			cod_moneda, fecha, nota1, nota2, total_debe, total_haber, estado_voucher, fecha_contable, usuario_creacion, 
			fecha_creacion, hora_creacion, usuario_modificacion, fecha_modificacion, hora_modificacion, fecha_proceso, nro_ejecucion )
			select compania, sucursal, ejercicio, periodoctb, asiento, comprobant, moneda, fecha, nota1, nota2, totdebe, 
			tothaber, stsvoucher, feccontab, usucrea, feccrea, horcrea, ultusumod, fecultmod, horultimod,
			fecha_proceso, nro_ejecucion = convert(int,substring(@fechaejecucion,10,10)) 			
			from #tvouch1f 
			 
			execute stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 0 
			truncate table stgdwaje.dbo.stg_tvouch1f
		end 
	end
	end try 
	begin catch 
			execute stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 1
			execute stgdwaje.dbo.conf_inserta_log_error_bd 				
	end catch 
end
