use [odsdwaje]
go
/****** object:  storedprocedure [dbo].[usp_tran_t_voucher_detalle]    script date: 3/3/2025 6:34:30 pm ******/
set ansi_nulls on
go
set quoted_identifier on
go
alter procedure [dbo].[usp_tran_t_voucher_detalle] @fechaejecucion varchar(20) = ''
/*   
sistema        : ajegroup_ssis_ecuador  
nombre         : usp_tran_t_voucher_detalle  
descripcion    : control historico de la tabla t_voucher_detalle  
autor          : daphne orihuela - cosapi  
fecha creacion : 13-05-2014  
-------------------------------------------------------------------------------------  
fecha       descripcion                                              usuario  
-------------------------------------------------------------------------------------   
*/  
as  
begin  
 declare @nombretabla varchar(50)  
    declare @valorvalidacion int   
    set @nombretabla = 't_voucher_detalle'  
  
 exec stgdwaje.dbo.conf_validar_tabla @nombretabla, @fechaejecucion, @valorvalidacion output    
 begin try 
  if @valorvalidacion = 1  
  begin   
     
   truncate table dbo.tmp_historico_voucher      
   insert into dbo.tmp_historico_voucher(compania, sucursal, ejercicio, periodoctb, asiento, comprobant, lineavou, ctaautomat, fecvocher, tipodocctb,  
   codauxilia, codcontab, centrocost, naturaleza, flgcancsol, flgcancdol, tipodoc, vounser, vounnum, transrefer,   
   nrodocref, nroregistr, fecoper, fecvcmto, nrocheque, tipocambio, impsol, impdolar, glosa, ordtrab, tipodocref,  
   voundocref, comprogto, flggtodist, flujoefec, saldo, gasto, motivo, estado, feccrea, horcrea, usucrea, fecultmod,  
   horultimod, ultusumod, categoria, canal, ciadoc, sucdoc, rfc, region, bd_origen, fecha_proceso, moneda,  
   nota1, nota2, tipcreacio, intercom, codauxper)  
   select compania, sucursal, ejercicio, periodoctb, asiento, comprobant, lineavou, ctaautomat, fecvocher, tipodocctb,  
   codauxilia, codcontab, centrocost, naturaleza, flgcancsol, flgcancdol, tipodoc, vounser, vounnum, transrefer,   
   nrodocref, nroregistr, fecoper, fecvcmto, nrocheque, tipocambio, impsol, impdolar, glosa, ordtrab, tipodocref,  
   voundocref, comprogto, flggtodist, flujoefec, saldo, gasto, motivo, estado, feccrea, horcrea, usucrea, fecultmod,  
   horultimod, ultusumod, categoria, canal, ciadoc, sucdoc, rfc, region, bd_origen, fecha_proceso, moneda,  
   nota1, nota2, tipcreacio, intercom, codauxper   
   from stgdwaje.dbo.stg_tvouch2f t   
  
   if @@rowcount = 0  
   begin  
    insert into stgdwaje.dbo.rej_tvouch2f(id_error, fecha_proceso, desc_error)   
    values (3, @fechaejecucion, 'stage sin data')  
    execute stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 0  
   end  
   else  
   begin   
       
    insert into stgdwaje.dbo.rej_tvouch2f (id_error, fecha_proceso, bd_origen, desc_error)  
    select 3, @fechaejecucion, a.descripcion, 'bd: ' + a.descripcion + ' vacia'  
    from stgdwaje..conf_tabla_dominio a  
    where id_grupo = 2 and id_orden > 0 and valor2 = 'activo'  
    and not exists(select 1 from dbo.tmp_historico_voucher b where a.descripcion = b.bd_origen)  
       
    delete t   
    from dbo.tmp_historico_voucher t   
    where exists(select 1 from dbo.t_voucher_eliminados te where t.compania = te.cod_compania   
    and t.sucursal = te.cod_sucursal and t.ejercicio = te.cod_ejercicio and t.periodoctb = te.cod_periodo   
    and t.asiento = te.asiento and t.comprobant = te.nro_comprobante)  
     
    select compania, sucursal, ejercicio, periodoctb, asiento, comprobant, lineavou, ctaautomat, fecvocher, tipodocctb,  
    codauxilia, codcontab, centrocost, naturaleza, flgcancsol, flgcancdol, tipodoc, vounser, vounnum, transrefer,   
    nrodocref, nroregistr, fecoper, fecvcmto, nrocheque, tipocambio, impsol, impdolar, glosa, ordtrab, tipodocref,  
    voundocref, comprogto, flggtodist, flujoefec, saldo, gasto, motivo, estado, feccrea, horcrea, usucrea, fecultmod,  
    horultimod, ultusumod, categoria, canal, ciadoc, sucdoc, rfc, region, bd_origen, fecha_proceso, moneda,  
    nota1, nota2, tipcreacio, intercom, codauxper,    
    id_error =  5,  
    desc_error = case when not exists (select 1 from dbo.t_voucher_cabecera m  
          where m.cod_compania = a.compania and m.cod_sucursal = a.sucursal and m.cod_ejercicio = a.ejercicio  
           and m.cod_periodo = a.periodoctb and m.asiento = a.asiento and m.nro_comprobante = a.comprobant)  
        then 'no existe foreign para t_voucher_cabecera'  
        when not exists (select 1 from dbo.m_periodo m where m.cod_compania = a.compania   
         and m.cod_ejercicio = a.ejercicio and m.cod_periodo = a.periodoctb)   
          then 'no existe foreign para m_periodo'  
        when not exists (select 1 from dbo.m_sucursal m where m.cod_compania = a.compania   
         and m.cod_sucursal = a.sucursal)   
          then 'no existe foreign para m_sucursal'   
        when not exists (select 1 from dbo.m_plan_cuentas m where m.cod_compania = a.compania   
         and m.cod_ejercicio = a.ejercicio and m.cod_cuenta_contable = a.codcontab)  
          then 'no existe foreign para m_plan_cuentas'  
        when not exists (select 1 from dbo.m_centro_costo m where m.cod_compania = a.compania   
         and m.cod_ejercicio = a.ejercicio and m.cod_centro_costo = a.centrocost)  
          then 'no existe foreign para m_centro_costo'  
       end  
    into #tvouch2f_inconsistencia  
    from dbo.tmp_historico_voucher a  
    where not exists (select 1 from dbo.t_voucher_cabecera m  
     where m.cod_compania = a.compania and m.cod_sucursal = a.sucursal and m.cod_ejercicio = a.ejercicio  
     and m.cod_periodo = a.periodoctb and m.asiento = a.asiento and m.nro_comprobante = a.comprobant)  
     or not exists (select 1 from dbo.m_periodo m where m.cod_compania = a.compania  
       and m.cod_ejercicio = a.ejercicio and m.cod_periodo = a.periodoctb)   
     or not exists (select 1 from dbo.m_sucursal m where m.cod_compania = a.compania   
        and m.cod_sucursal = a.sucursal)   
     or not exists (select 1 from dbo.m_plan_cuentas m where m.cod_compania = a.compania   
        and m.cod_ejercicio = a.ejercicio and m.cod_cuenta_contable = a.codcontab)  
     or not exists (select 1 from dbo.m_centro_costo m where m.cod_compania = a.compania   
        and m.cod_ejercicio = a.ejercicio and m.cod_centro_costo = a.centrocost)  
  
    insert into stgdwaje.dbo.rej_tvouch2f (compania, sucursal, ejercicio, periodoctb, asiento, comprobant, lineavou,   
    ctaautomat, fecvocher, tipodocctb, codauxilia, codcontab, centrocost, naturaleza, flgcancsol, flgcancdol, tipodoc,  
    vounser, vounnum, transrefer, nrodocref, nroregistr, fecoper, fecvcmto, nrocheque, tipocambio, impsol, impdolar,  
    glosa, ordtrab, tipodocref, voundocref, comprogto, flggtodist, flujoefec, saldo, gasto, motivo, estado, feccrea,  
    horcrea, usucrea, fecultmod, horultimod, ultusumod, categoria, canal, ciadoc, sucdoc, rfc, region, bd_origen, fecha_proceso,  
    moneda, nota1, nota2, tipcreacio, intercom, codauxper, id_error, desc_error)  
    select compania, sucursal, ejercicio, periodoctb, asiento, comprobant, lineavou,   
    ctaautomat, fecvocher, tipodocctb, codauxilia, codcontab, centrocost, naturaleza, flgcancsol, flgcancdol, tipodoc,  
    vounser, vounnum, transrefer, nrodocref, nroregistr, fecoper, fecvcmto, nrocheque, tipocambio, impsol, impdolar,  
    glosa, ordtrab, tipodocref, voundocref, comprogto, flggtodist, flujoefec, saldo, gasto, motivo, estado, feccrea,  
    horcrea, usucrea, fecultmod, horultimod, ultusumod, categoria, canal, ciadoc, sucdoc, rfc, region, bd_origen, fecha_proceso,  
    moneda, nota1, nota2, tipcreacio, intercom, codauxper, id_error, desc_error  
    from #tvouch2f_inconsistencia  
       
    delete t  
    from dbo.tmp_historico_voucher t  
    where exists (select 1 from #tvouch2f_inconsistencia a  
      where t.compania = a.compania 
      and t.sucursal = a.sucursal 
      and t.ejercicio = a.ejercicio  
       and t.periodoctb = a.periodoctb 
       and t.asiento = a.asiento 
       and t.comprobant = a.comprobant  
       and t.lineavou = a.lineavou 
       and t.ctaautomat = a.ctaautomat)  
   
    select *  
    into #tvouch2f_duplicado  
    from dbo.tmp_historico_voucher a  
    where exists (select 1 from dbo.tmp_historico_voucher b  
      where b.compania = a.compania and b.sucursal = a.sucursal and b.ejercicio = a.ejercicio  
       and b.periodoctb = a.periodoctb and b.asiento = a.asiento and b.comprobant = a.comprobant  
       and b.lineavou = a.lineavou and b.ctaautomat = a.ctaautomat  
    group by compania, sucursal, ejercicio, periodoctb, asiento, comprobant, lineavou, ctaautomat  
    having count(*) > 1)  
  
    insert into stgdwaje.dbo.rej_tvouch2f (compania, sucursal, ejercicio, periodoctb, asiento, comprobant, lineavou,   
    ctaautomat, fecvocher, tipodocctb, codauxilia, codcontab, centrocost, naturaleza, flgcancsol, flgcancdol, tipodoc,  
    vounser, vounnum, transrefer, nrodocref, nroregistr, fecoper, fecvcmto, nrocheque, tipocambio, impsol, impdolar,  
    glosa, ordtrab, tipodocref, voundocref, comprogto, flggtodist, flujoefec, saldo, gasto, motivo, estado, feccrea,  
    horcrea, usucrea, fecultmod, horultimod, ultusumod, categoria, canal, ciadoc, sucdoc, rfc, region, bd_origen, fecha_proceso,  
    moneda, nota1, nota2, tipcreacio, intercom, codauxper, id_error, desc_error)  
    select compania, sucursal, ejercicio, periodoctb, asiento, comprobant, lineavou,   
    ctaautomat, fecvocher, tipodocctb, codauxilia, codcontab, centrocost, naturaleza, flgcancsol, flgcancdol, tipodoc,  
    vounser, vounnum, transrefer, nrodocref, nroregistr, fecoper, fecvcmto, nrocheque, tipocambio, impsol, impdolar,  
    glosa, ordtrab, tipodocref, voundocref, comprogto, flggtodist, flujoefec, saldo, gasto, motivo, estado, feccrea,  
    horcrea, usucrea, fecultmod, horultimod, ultusumod, categoria, canal, ciadoc, sucdoc, rfc, region, bd_origen, fecha_proceso,  
    moneda, nota1, nota2, tipcreacio, intercom, codauxper, 2, 'clave duplicada'  
    from #tvouch2f_duplicado  
  
    delete t  
    from dbo.tmp_historico_voucher t  
    where exists (select 1 from #tvouch2f_duplicado a  
      where t.compania = a.compania and t.sucursal = a.sucursal and t.ejercicio = a.ejercicio  
       and t.periodoctb = a.periodoctb and t.asiento = a.asiento and t.comprobant = a.comprobant  
       and t.lineavou = a.lineavou and t.ctaautomat = a.ctaautomat)  
            
    delete t  
    from dbo.t_voucher_detalle t  
    where exists (select 1 from dbo.tmp_historico_voucher a  
      where t.cod_compania = a.compania and t.cod_ejercicio = a.ejercicio and t.cod_periodo = a.periodoctb)  
         
    insert into dbo.t_voucher_detalle (cod_compania, cod_sucursal, cod_ejercicio, cod_periodo, asiento, nro_comprobante, linea_voucher,   
    cta_automatica, fecha_voucher, cod_contable, naturaleza, tipo_documento, tipo_cambio, importe_sol, importe_dolar, glosa,   
    usuario_creacion, fecha_creacion, hora_creacion, usuario_modificacion, fecha_modificacion, hora_modificacion,    
    codigo_auxiliar, nro_serie, nro_documento, transferencia_refencia, nro_documento_referencia, cod_centro_costo, cod_moneda,   
    cod_compania_doc, cod_sucursal_doc, nota1, nota2, tipo_creacion, intercom, cod_auxiliar_persona,  
    nro_registro, fecha_operacion, tipo_documento_ref, documento_ref, fecha_proceso, nro_ejecucion)   
    select compania, sucursal, ejercicio, periodoctb, asiento, comprobant, lineavou, ctaautomat, fecvocher, codcontab,  
    naturaleza, tipodoc, tipocambio, impsol, impdolar, glosa, usucrea, feccrea, horcrea, ultusumod, fecultmod, horultimod,  
    codauxilia, vounser, vounnum, transrefer, nrodocref, centrocost, moneda, ciadoc, sucdoc, nota1, nota2,  
    tipcreacio, intercom, codauxper, nroregistr, fecoper, tipodocref, voundocref, fecha_proceso, nro_ejecucion = convert(int,substring(@fechaejecucion,10,10))   
    from dbo.tmp_historico_voucher t   
     
    exec stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 0  
    truncate table stgdwaje.dbo.stg_tvouch2f   
   end  
  end  
 end try  
 begin catch  
  exec stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla,@fechaejecucion,1  
  exec stgdwaje.dbo.conf_inserta_log_error_bd  
 end catch  
end
