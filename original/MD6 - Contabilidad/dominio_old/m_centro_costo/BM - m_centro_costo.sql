use [odsdwaje]
go
/****** object:  storedprocedure [dbo].[usp_tran_m_centro_costo]    script date: 2/27/2025 5:28:30 pm ******/
set ansi_nulls on
go
set quoted_identifier on
go
alter procedure [dbo].[usp_tran_m_centro_costo] @fechaejecucion varchar(20) = ''  
/*   
sistema        : ajegroup_ssis_ecuador  
nombre         : usp_tran_m_centro_costo  
descripción    : insertar y actualizar registros a m_centro_costo  
autor          : daphne orihuela - cosapi  
fecha creación : 20-03-2014  
-------------------------------------------------------------------------------------  
fecha       descripción                                              usuario  
-------------------------------------------------------------------------------------  
14/07/2016  se agregan las columnas de codigo y desc corporativos  mcollazos  
22/07/2019  se modifica el centro de costo corporativo según 
			configuración											gramirez
  
*/   
  
as  
begin  
 declare @nombretabla varchar(50)  
 declare @valorvalidacion int  
 declare @flagcorporativo int
      
    set @nombretabla = 'm_centro_costo'  

      
begin try  
	--ejecutar si se encuentra validado la tabla para su ejecucion  
	exec stgdwaje.dbo.conf_validar_tabla @nombretabla, @fechaejecucion, @valorvalidacion output  
	------- insertar registros default ---------------  
	--------------------------------------------------  
	select m.cod_compania, '000000000' cod_centro_costo, t.cod_ejercicio, 'centro costo default' nombre, '000' cod_area, '00' cod_gerencia, 'a' tipo, '' tipo_almacen, 'i' estado, '' cod_centro_costo_corp, '' nombre_corp  
	into #m_centro_costo_default
	from m_compania m  
	cross join m_ejercicio t    
	where m.cod_compania = t.cod_compania  
  
	insert into m_gerencia (cod_compania, cod_gerencia, cod_ejercicio, cod_sucursal, nombre, estado) 
	select distinct cod_compania, cod_gerencia, cod_ejercicio, '00' cod_sucursal, nombre, estado 
	from #m_centro_costo_default p 
	where not exists(select 1 from m_gerencia t where p.cod_compania = t.cod_compania 
	and p.cod_ejercicio = t.cod_ejercicio and p.cod_gerencia = t.cod_gerencia) 

	insert into m_area (cod_compania, cod_area, cod_ejercicio, cod_gerencia, cod_sucursal, descripcion, estado)
	select distinct cod_compania, cod_area, cod_ejercicio, cod_gerencia, '00' cod_sucursal, nombre, estado 
	from #m_centro_costo_default p 
	where not exists(select 1 from m_area t where p.cod_compania = t.cod_compania 
	and p.cod_ejercicio = t.cod_ejercicio and p.cod_area = t.cod_area)

	insert into m_centro_costo(cod_compania, cod_centro_costo, cod_ejercicio, nombre, cod_area, cod_gerencia, tipo, tipo_almacen, estado, cod_centro_costo_corp, nombre_corp  )   
	select cod_compania, cod_centro_costo, cod_ejercicio, nombre, cod_area, cod_gerencia, tipo, tipo_almacen, estado, cod_centro_costo_corp, nombre_corp  
	from #m_centro_costo_default p   
	where not exists(select 1 from m_centro_costo t where p.cod_compania = t.cod_compania   
	and p.cod_ejercicio = t.cod_ejercicio and p.cod_centro_costo = t.cod_centro_costo)  
	 
  --------------------------------------------------  
  if @valorvalidacion = 1  
  begin  
   
   select @flagcorporativo = valor1
     from stgdwaje..conf_tabla_dominio
	where id_grupo = 30
	  and id_orden = 1
   
   
   select compania, sucursal, centrocost, nombre, uninegocio, direccion, ciudad, area,  
   flgtipo, tipalmacen, flgalmaloc, gerencia, gironegoci, situacion, fecsituac, flgtrabcur,  
   fecinicio, feccierre, estado, defcmle, ejercicio, 
   case when isnull(@flagcorporativo, 1) = 1 then centrocost else isnull(ccostcorp,'') end ccostcorp,
   case when isnull(@flagcorporativo, 1) = 1 then nombre else isnull(nombrecorp,'') end nombrecorp,
   feccrea, fecultmod, horcrea, horultimod, ultusumod, usucrea, bd_origen, fecha_proceso  
   into #tccost1f      
   from stgdwaje.dbo.stg_tccost1f    
       
   if @@rowcount = 0  
   begin  
    insert into stgdwaje.dbo.rej_tccost1f(id_error, fecha_proceso, desc_error)   
    values (3, @fechaejecucion, 'stage sin data')       
    exec stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 0  
   end  
   else  
   begin     
    -- validar bd origen vacia  
    insert into stgdwaje.dbo.rej_tccost1f(id_error, fecha_proceso ,bd_origen, desc_error)  
    select valor = 3, fecha_ejecucion = @fechaejecucion, a.descripcion,  
    desc_error = 'bd: ' + a.descripcion + ' vacia'  
    from stgdwaje..conf_tabla_dominio a  
    where id_grupo = 2 and id_orden > 0 and valor2 ='activo' and valor4 = 'magic'  
    and not exists (select 1 from #tccost1f b where a.descripcion = b.bd_origen)  
   
    ---------------------------------------------------------------  
    update p  
    set p.gerencia = '00',  
    p.area = '000'  
    from #tccost1f p  
    where not exists(select 1 from dbo.m_area t where p.compania = t.cod_compania   
    and p.ejercicio = t.cod_ejercicio and p.area = t.cod_area)  
      
    update p  
    set p.gerencia = '00'   
    from #tccost1f p  
    where not exists(select 1 from dbo.m_gerencia t where p.compania = t.cod_compania   
    and p.ejercicio = t.cod_ejercicio and p.gerencia = t.cod_gerencia) 
    
    update p  
    set p.sucursal = '00'   
    from #tccost1f p  
    where not exists(select 1 from dbo.m_sucursal t where p.compania = t.cod_compania   
    and p.sucursal = t.cod_sucursal) 
    ---------------------------------------------------------------   
      
    select compania, sucursal, centrocost, nombre,   
    uninegocio, direccion, ciudad, area,  
    flgtipo, tipalmacen, flgalmaloc, gerencia, gironegoci, situacion, fecsituac, flgtrabcur,  
    fecinicio, feccierre, estado, defcmle, ejercicio, ccostcorp,  
    fecha_proceso,   
    feccrea, fecultmod, horcrea, horultimod, ultusumod, usucrea,   
    bd_origen, nombrecorp,  
    id_error = 5,  
    desc_error = case when nombre = ''   
          then 'campo vacio en nombre'   
          when not exists (select 1 from m_ejercicio e where a.compania = e.cod_compania and a.ejercicio = e.cod_ejercicio)  
          then 'no existe foreign para m_ejercicio'  
          when not exists (select 1 from m_area c where a.compania = c.cod_compania and a.area = c.cod_area and a.ejercicio = c.cod_ejercicio)  
          then 'no existe foreign para m_area'   
          when not exists (select 1 from m_gerencia c where a.compania = c.cod_compania and a.gerencia = c.cod_gerencia and a.ejercicio = c.cod_ejercicio)  
          then 'no existe foreign para m_gerencia'    
          end,  
    flg_ajuste = case when not exists (select 1 from m_ejercicio m where m.cod_compania = a.compania   
         and m.cod_sucursal = a.sucursal and m.cod_ejercicio = a.ejercicio)  
          then 1  
         else 0  
        end              
    into #tccost1f_inconsistencia --drop table #tccost1f_inconsistencia   
    from #tccost1f a  
    where nombre = ''  
    or not exists (select 1 from m_ejercicio e where a.compania = e.cod_compania and a.ejercicio = e.cod_ejercicio)  
    or not exists (select 1 from m_area c where a.compania = c.cod_compania and a.area = c.cod_area and a.ejercicio = c.cod_ejercicio)  
    or not exists (select 1 from m_gerencia c where a.compania = c.cod_compania and a.gerencia = c.cod_gerencia and a.ejercicio = c.cod_ejercicio)  
    --------  
    ----insertar registros ajuste   
    --------       
    select distinct compania cod_compania,   
    sucursal cod_sucursal,   
    ejercicio cod_ejercicio,   
    'ejercicio default' descripcion,  
    'i' estado  
    into #m_ejercicio   
    from #tccost1f_inconsistencia  
    where flg_ajuste = 1  
      
    insert into dbo.m_ejercicio(cod_compania, cod_sucursal, cod_ejercicio, descripcion, estado)  
    select cod_compania, cod_sucursal, cod_ejercicio, descripcion, estado   
    from #m_ejercicio t  
    where not exists(select 1 from dbo.m_ejercicio m where t.cod_compania = m.cod_compania   
    /*and m.cod_sucursal = t.cod_sucursal*/ and m.cod_ejercicio = t.cod_ejercicio)  
    --------  
    --------  
       
    insert into stgdwaje.dbo.rej_tccost1f (compania, sucursal, centrocost, nombre, uninegocio, direccion, ciudad, area,  
    flgtipo, tipalmacen, flgalmaloc, gerencia, gironegoci, situacion, fecsituac, flgtrabcur,  
    fecinicio, feccierre, estado, defcmle, ejercicio, ccostcorp, fecha_proceso,   
    feccrea, fecultmod, horcrea, horultimod, ultusumod, usucrea, bd_origen, nombrecorp, id_error, desc_error)  
    select compania, sucursal, centrocost, nombre, uninegocio, direccion, ciudad, area,  
    flgtipo, tipalmacen, flgalmaloc, gerencia, gironegoci, situacion, fecsituac, flgtrabcur,  
    fecinicio, feccierre, estado, defcmle, ejercicio, ccostcorp, fecha_proceso,   
    feccrea, fecultmod, horcrea, horultimod, ultusumod, usucrea, bd_origen, nombrecorp, id_error, desc_error  
    from #tccost1f_inconsistencia  
    where flg_ajuste = 0  
      
    -- deleteando los transferido a las inconsistencias  
    delete p   
    from #tccost1f p  
    where exists (select 1 from #tccost1f_inconsistencia t   
      where p.compania = t.compania   
      and p.centrocost = t.centrocost  
      and p.ejercicio = t.ejercicio  
      and t.flg_ajuste = 0)  
         
    --limpieza de duplicados  
    select *   
    into #tccost1f_duplicados   
    from #tccost1f a  
    where exists (select 1 from #tccost1f b  
        where a.compania = b.compania and a.centrocost = b.centrocost and   
        a.ejercicio = b.ejercicio  
        group by compania, centrocost, ejercicio   
        having count(*) > 1)  
  
    insert into stgdwaje.dbo.rej_tccost1f (compania, sucursal, centrocost, nombre, uninegocio, direccion, ciudad, area,  
    flgtipo, tipalmacen, flgalmaloc, gerencia, gironegoci, situacion, fecsituac, flgtrabcur,  
    fecinicio, feccierre, estado, defcmle, ejercicio, ccostcorp, fecha_proceso,   
    feccrea, fecultmod, horcrea, horultimod, ultusumod, usucrea, bd_origen, nombrecorp, id_error, desc_error)  
    select compania, sucursal, centrocost, nombre, uninegocio, direccion, ciudad, area,  
    flgtipo, tipalmacen, flgalmaloc, gerencia, gironegoci, situacion, fecsituac, flgtrabcur,  
    fecinicio, feccierre, estado, defcmle, ejercicio, ccostcorp, fecha_proceso,   
    feccrea, fecultmod, horcrea, horultimod, ultusumod, usucrea, bd_origen, nombrecorp, 2, 'clave duplicada'  
    from #tccost1f_duplicados  
      
    delete p   
    from #tccost1f p  
    where exists (select 1 from #tccost1f_duplicados a   
     where a.compania = p.compania   
     and a.centrocost = p.centrocost   
     and a.ejercicio = p.ejercicio)  
     
    /*insertar registro nuevo*/  
    insert m_centro_costo(cod_compania, cod_centro_costo, cod_ejercicio, nombre,  
    cod_area, cod_gerencia, tipo, tipo_almacen, estado, cod_centro_costo_corp, nombre_corp)  
    select compania, centrocost, ejercicio, nombre,   
    area, gerencia, flgtipo, tipalmacen, estado, ccostcorp, nombrecorp  
    from #tccost1f t  
    where not exists (select 1 from m_centro_costo d where d.cod_compania = t.compania   
     and d.cod_centro_costo = t.centrocost and d.cod_ejercicio = t.ejercicio)  
      
    /*actualizar registo*/  
    update t   
    set  nombre = a.nombre,  
    cod_area = a.area,  
    cod_gerencia = a.gerencia,  
    tipo = a.flgtipo,  
    tipo_almacen = a.tipalmacen,  
    estado = a.estado,  
    cod_centro_costo_corp = isnull(a.ccostcorp,''),  
    nombre_corp = isnull(a.nombrecorp,'')  
    from m_centro_costo t  
    inner join #tccost1f a on t.cod_compania = a.compania   
        and t.cod_centro_costo = a.centrocost   
        and t.cod_ejercicio =a.ejercicio  
    where (t.nombre <> a.nombre   
       or t.cod_area <> a.area   
       or t.cod_gerencia <> a.gerencia  
       or t.tipo <> a.flgtipo  
       or t.tipo_almacen <> a.tipalmacen  
       or t.estado <> a.estado  
       or isnull(t.cod_centro_costo_corp,'') <> isnull(a.ccostcorp,'')  
       or isnull(t.nombre_corp,'') <> isnull(a.nombrecorp,''))  
          
    -- actualizando el estado de la tabla matriz  
    exec stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 0  
       
    truncate table stgdwaje.dbo.stg_tccost1f  
   end  
  end  
    
end try  
   
begin catch  
  exec stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 1  
  exec stgdwaje.dbo.conf_inserta_log_error_bd   
end catch  
  
end  
