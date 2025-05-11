use [odsdwaje]
go
/****** object:  storedprocedure [dbo].[usp_tran_t_voucher_eliminados]    script date: 3/3/2025 6:35:35 pm ******/
set ansi_nulls on
go
set quoted_identifier on
go
alter procedure [dbo].[usp_tran_t_voucher_eliminados] @fechaejecucion varchar(20) = ''
/*   
sistema        : ajegroup_ssis_ecuador  
nombre         : usp_tran_t_voucher_eliminados  
descripción    : control historico de la tabla t_voucher_eliminados  
autor          : ivan mejia - cosapi  
fecha creación : 02-09-2014  
-------------------------------------------------------------------------------------  
fecha       descripción                                              usuario  
-------------------------------------------------------------------------------------  
*/   
as  
begin 
	set nocount off  
	declare @nombretabla varchar(50)  
	declare @valorvalidacion int  
	set @nombretabla = 't_voucher_eliminados'    
	begin try   
	if @valorvalidacion = 1  
	begin 
		select compania, sucursal, ejercicio, periodoctb, asiento, comprobant,  
		fechavouc, usucrea, feccrea, horcrea, ultusumod, ultfecmod, ulthormod,  
		bd_origen, fecha_proceso 
		into #tvouch6f 
		from stgdwaje.dbo.stg_tvouch6f t  
		-- nuevos registros  
		delete p 
		from dbo.t_voucher_eliminados p
		where exists(select 1 from #tvouch6f t 
		where p.cod_compania = t.compania 
		and p.cod_sucursal = t.sucursal 
		and p.cod_ejercicio = t.ejercicio 
		and p.cod_periodo = t.periodoctb 
		and p.asiento = t.asiento 
		and p.nro_comprobante = t.comprobant)
		
		insert into dbo.t_voucher_eliminados(cod_compania, cod_sucursal, cod_ejercicio, 
		cod_periodo, asiento, nro_comprobante, fecha,usuario_creacion, fecha_creacion, 
		hora_creacion, usuario_modificacion, fecha_modificacion, hora_modificacion,  
		fecha_proceso, nro_ejecucion)  
		select compania, sucursal, ejercicio, periodoctb, asiento, comprobant,  
		fechavouc,usucrea, feccrea, horcrea, ultusumod, ultfecmod, ulthormod,  
		fecha_proceso, nro_ejecucion = convert(int,substring(@fechaejecucion,10,10))     
		from #tvouch6f 
		
		execute stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 0   
		truncate table stgdwaje.dbo.stg_tvouch6f  
	end  
	end try  
	begin catch   
	   execute stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 1  
	   execute stgdwaje.dbo.conf_inserta_log_error_bd   
	end catch  
end  
