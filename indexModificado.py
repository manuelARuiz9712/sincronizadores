import pyodbc
import requests
import datetime
import json

server = '192.168.1.252\SQL2008, 60313'
database = 'POSManu'
username = 'DeveloperWeb2'
password = 'Web163BDis'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
print("Connected")
cursor = cnxn.cursor()

UrlApi = "https://pitagoras.dismelltda.com/apitest/public"


#cursor.execute("select top 100*  from Ex_POS_UsuarioServicio")
#rows = cursor.fetchall()
#print(rows)



def GetDateFacturacion():
    x = datetime.datetime.now()
    return ""+x.strftime("%Y")+x.strftime("%m")+x.strftime("%d") 

def NonHumanFacturation (Pedido,Turno,Consecutivo,IdDocumento,ExtraData):

    QueryItems = ""
    ConceptosIcos = 0
    Fecha  =GetDateFacturacion()
    IdentificacionUsuario = Pedido['Info']["NumeroIdentificacion"]
    TotalIvaDef = 0
    PLaza = ''
    Banco == ''


    if Pedido['Info']['NombreTipoPago']  == 'Datafono':
        PLaza = 'CTG'
  



    for ItemPedido in Pedido['items']:

        if ItemPedido["Tipo"] == 'Producto':


            TotalIva =  ((  int( ItemPedido["IVA"]  )/100) * ItemPedido['PrecioBase'])* int( ItemPedido['Cantidad'])
            TotalIvaDef = TotalIvaDef + TotalIva 
            PrecioUnidad =  ItemPedido['PrecioBase'] + TotalIva
            PrecioTotal = ItemPedido['PrecioBase'] * int( ItemPedido['Cantidad'] ) 

            ConceptosIcos += int( ItemPedido['ICO'] )*int( ItemPedido['Cantidad'] )
            QueryItems += """

Exec spItemsDocumentos  @Op = 'I', @IDEN_Documentos =  @Iden_Documento_Select, @Cantidad = {Cantidad}, @PrecioUnidad = {PrecioUnidad}, @PrecioTotal = {PrecioTotal}, @CostoTotal = 0,
@PorcentajeIVA = {IVA}, @PorcentajeDescuento = 0, @PorcentajeSobreDescuento = 0, @TotalDescuento = 0, @TotalIVA = {TotalIva}, @IdArticulo = {IdArticulo}, @VentaPorEmpaque = 0,
@Bodega = '13', @Ubicacion = '', @Lote = '0', @Clasificacion = 0, @Serie = '', @PrecioTotalOtraMoneda = 0, @PrecioUnidadOtraMoneda = 0, @Requerido = 1,
@SobreDescuento = 0, @Detalle = '', @PorcentajeINC = 0, @TotalINC = 0, @Serial = null
            
            """.format(TotalIva=TotalIva,Cantidad=ItemPedido['Cantidad'],PrecioUnidad=PrecioUnidad,PrecioTotal=PrecioTotal,IVA=ItemPedido['IVA'],IdArticulo=ItemPedido['Codigo'])

        else:

            for SubItem in ItemPedido["Composicion"]:
                UnidadesTotal = int( SubItem['Cantidad'] )  * int( ItemPedido['Cantidad'] )
                TotalIva =    (int( SubItem["Iva"]  )/100) * SubItem['PrecioBase'] * UnidadesTotal
                TotalIvaDef = TotalIvaDef + TotalIva 
                PrecioUnidad =  SubItem['PrecioBase'] + TotalIva
                PrecioTotal = SubItem['PrecioBase'] * UnidadesTotal
                

                ConceptosIcos += int( SubItem['Ico'] ) * UnidadesTotal

                QueryItems += """

Exec spItemsDocumentos  @Op = 'I', @IDEN_Documentos =  @Iden_Documento_Select, @Cantidad = {Cantidad}, @PrecioUnidad = {PrecioUnidad}, @PrecioTotal = {PrecioTotal}, @CostoTotal = 0,
@PorcentajeIVA = {IVA}, @PorcentajeDescuento = 0, @PorcentajeSobreDescuento = 0, @TotalDescuento = 0, @TotalIVA = {TotalIva}, @IdArticulo = {IdArticulo}, @VentaPorEmpaque = 0,
@Bodega = '13', @Ubicacion = '', @Lote = '0', @Clasificacion = 0, @Serie = '', @PrecioTotalOtraMoneda = 0, @PrecioUnidadOtraMoneda = 0, @Requerido = 1,
@SobreDescuento = 0, @Detalle = '', @PorcentajeINC = 0, @TotalINC = 0, @Serial = null
            
            """.format(TotalIva=TotalIva,Cantidad= UnidadesTotal,PrecioUnidad=PrecioUnidad,PrecioTotal=PrecioTotal,IVA=SubItem['Iva'],IdArticulo=SubItem['Codigo'])


    #print( QueryItems ,"\n")  

    QueryConsecutivos = """
    EXEC spGenerarMovimientoInventario @TipoDocumento = 1,@Consecutivo=@Consecutivo_Select
EXEC spContabilizador @TipoDocumento=1,@Consecutivo=@Consecutivo_Select
EXEC SpSincronizarVenta @TipoDocumento=1,@Consecutivo=@Consecutivo_Select
---------------------------------------------
IF @@TRANCOUNT>0 BEGIN COMMIT TRAN END
    
    """


  

    QueryMedioPago = """
    
    
    Exec spPagosDocumentos  @Op='I',@IDen_Documentos=@Iden_Documento_Select,@FormaPago='{MedioPago}',@Valor={ValorTotalPedido},@Cambio=0,@Comision=0,@Retencion=0,@IvaInformado={TotalIvaDef},@ReteIVA=0,
@Vencimiento='{Fecha}',@Cliente='99        ',@Banco='{Banco}',@Plaza='{Plaza}',@DocumentoPago='{DocumentoPago}',@EsUnAnticipo=0
    
    """.format(DocumentoPago= Pedido['Info']['CodigoTransaccion'],Plaza=PLaza,Banco=Pedido['Info']['Banco'],IdDocumento =IdDocumento,ValorTotalPedido =float( Pedido['Info']['ValorPedido'] ),Fecha=Fecha,TotalIvaDef=TotalIvaDef,MedioPago=Pedido['Info']["CodigoPago"])
    QueryConeptiosIcos = """
    
    Exec spConceptosDocumentos @Op='I', @IDEN_Documentos=@Iden_Documento_Select, @Concepto='44', @Cantidad=1, @Valor={ValorIco}, @BaseRetencion=0, @PorcentajeImpuesto=0, @ValorUnidad1=0,
@ValorUnidad2=0, @ValorUnidad3=0, @cliente='          ', @Proveedor='          ', @DocumentoCartera='', @TipoDocumentoCartera='', @Vencimiento='20200403',
@Referencia='', @Vendedor='', @Moneda='', @DocumentoCaja='', @Fecha='{Fecha}', @Banco='   ', @plaza='   ', @AuxiliarAbierto='                ',
@CentroCosto='', @Autorizacion='          ', @ReferenciaCaja='          ', @Tercero='99                       ', @ItemsContable='', @ValorEnOtraMoneda={ValorIco},
@SubTotal={ValorIco}, @TotalDescuento=0, @Total={ValorIco}, @TotalIVA=0, @TotalConcepto={ValorIco}, @PorcentajeIVA=0, @PorcentajeDcto=0, @Detalle='IMPOCONSUMO LICORES',
@IdenTiquetera = 0, @ConsecutivoTiquetera = 0
    
    """.format(ValorIco=ExtraData['TotalIco'],Fecha=Fecha)

    QueryConeptiosDomicilio = """
    
    Exec spConceptosDocumentos @Op='I', @IDEN_Documentos=@Iden_Documento_Select, @Concepto='{IdConcepto}', @Cantidad=1, @Valor={ValorIco}, @BaseRetencion=0, @PorcentajeImpuesto=0, @ValorUnidad1=0,
@ValorUnidad2=0, @ValorUnidad3=0, @cliente='          ', @Proveedor='          ', @DocumentoCartera='', @TipoDocumentoCartera='', @Vencimiento='20200403',
@Referencia='', @Vendedor='', @Moneda='', @DocumentoCaja='', @Fecha='{Fecha}', @Banco='   ', @plaza='   ', @AuxiliarAbierto='DOMICILIOS      ',
@CentroCosto='', @Autorizacion='          ', @ReferenciaCaja='          ', @Tercero='99                       ', @ItemsContable='', @ValorEnOtraMoneda={ValorIco},
@SubTotal={ValorIco}, @TotalDescuento=0, @Total={ValorIco}, @TotalIVA=0, @TotalConcepto={ValorIco}, @PorcentajeIVA=0, @PorcentajeDcto=0, @Detalle='{DetalleConcepto}',
@IdenTiquetera = 0, @ConsecutivoTiquetera = 0
    
    """.format(ValorIco=Pedido['Info']['ValorConceptoDomicilio'],Fecha=Fecha,IdConcepto=Pedido['Info']['IdConceptoDismel'],DetalleConcepto=Pedido['Info']['NombreConcepto'])    

    QueryIdentificacionUsuario = """
    
    Exec spPOS_ManejoDeTerceros  @xOperacion = 'IR', @xCaja = '01', @xIdentificacion = '99-{DocumentoItem}', @xTipoTercero = '0', @xTipoIdentificacion = null,
@xNombre1 = null, @xNombre2 = null, @xApellido1 = null, @xApellido2 = null, @xTipoRazonSocial = 'NA'
    
    """.format(DocumentoItem=IdentificacionUsuario)



    LargeConsulta = """ 
BEGIN TRAN
Declare @Error Int
Declare @Iden_Documento_Select Numeric
Declare @Consecutivo_Select Numeric
Declare @Comprobante_Select Varchar(10)
Set @Error = -1

---------------------------------------------CABECERA DE DOCUMENTO-------------------------------------------------------------------
Execute @Error = spDocumentos  @Op = 'I', @Turno = {TurnoPedido}, @Estado = 'Procesado', @Fecha = '{FechaPedido} 00:00:00', @TipoDocumento = 1,
@Moneda = 'EFE', @TasaCambio = 1, @Cliente = '99        ', @Vendedor = '82 ', @UsuariodeServicio = '99-{IdentificacionUsuario}', @NombreUsuariodeServicio = 'Turista   ',
@TelefonoUsuariodeServicio = '1', @direccionUsuarioServicio = '.', @Identificacion = '{IdentificacionUsuario}', @FechaNacimiento = '19000101 00:00:00',
@PrestadordeServicio = null, @NombrePrestadordeServicio = null, @NroAutorizacion = null, @Fuente = '51', @Inicio = '00', @Comprobante = null,
@DescuentoComercial = 0, @Cargo1 = 0, @cargo2 = 0, @cargo3 = 0, @Usuario = 117, @AutorizadoPor = 0, @ValorDevuelto = 0, @Cupo = 1, @DocumentoconPendiente = 0,
@Anotaciones = null, @DocumentoDevuelto = 0, @ConsecutivoRelacionado = 0, @ValorAnticipo = 0, @PlanSepare_Consecutivo = null,
@PlanSepare_Consecutivo_Pago = null, @PlanSepare_Valor = 0, @Iden_Documento_Select = @Iden_Documento_Select OUTPUT,
@Consecutivo_Select = @Consecutivo_Select OUTPUT, @Comprobante_Select = @Comprobante_Select OUTPUT, @ModalidadesVentas = null, @Asociado = null

{ItesmPedidoQuery}

{ConceptosQuery}
{ConceptosDomiclioQuery}

{MedioPagoQuery}

{IdentificacionQuery}

{QueryConsecutivosEx}



    """.format(MedioPago=Pedido['Info']["CodigoPago"],ConceptosDomiclioQuery=QueryConeptiosDomicilio,QueryConsecutivosEx=QueryConsecutivos,IdentificacionUsuario=IdentificacionUsuario,TurnoPedido=Turno,FechaPedido=Fecha,ItesmPedidoQuery=QueryItems,MedioPagoQuery=QueryMedioPago,ConceptosQuery=QueryConeptiosIcos,IdentificacionQuery=QueryIdentificacionUsuario)
    #print("PEDIDO::","\n",LargeConsulta)
    return LargeConsulta

    




def QueryExistUsuarios(identificacion):
    return "select Codigo,Identificacion  from Ex_POS_UsuarioServicio where Identificacion = '{}'".format(identificacion)

def QueryInsertUsuario(Identificacion,Nombre,Direccion,Telefono):
    CodigoIdentificacion = '99-'+Identificacion
    return  "insert into Ex_POS_UsuarioServicio(Codigo,Nombre,Identificacion,Direccion,GrupoUsuarioServicio,Telefono) values('{}','{}','{}','{}','{}','{}')".format(CodigoIdentificacion,Nombre,Identificacion,Direccion,'vc99',Telefono)
    
def QueryGetTurno():
    return "select IDEN from Turno order by IDEN desc"

def QueryDocumentoInfo():
    return "select IDEN,Consecutivo from Documentos order by IDEN desc"    


def ComprobarExistencias(items):

    EstadoComprobado = True
    TotalIva  = 0
    TotalIco = 0
    Respuesta = {
        "TotalIva":0,
        "TotalIco":0,
        "CanFacure":True
    }

    for itemPedido in items:

        if  itemPedido['Tipo'] == 'Actividad':

            for subItems in itemPedido["Composicion"]:
                cursor.execute("""select Articulo.IdArticulo,Articulo.Nombre,Existencia.Existencias from Articulo
            inner join Existencia on Articulo.IdArticulo = Existencia.Articulo
            where Existencia.Bodega = 13 and IdArticulo = {}""".format(subItems['Codigo']))



                ResultExistencias = cursor.fetchone()

                if  ResultExistencias == None:
                    EstadoComprobado = False
                    break
                else:
                    if  int( subItems['Cantidad']) <=  int( ResultExistencias[2] ) :
                    
                        TotalIva =  TotalIva +  ((int( subItems['Iva'] )/100 * subItems['PrecioBase'])*subItems["Cantidad"])*subItems['Cantidad']
                        TotalIco = TotalIco +   (int( subItems['Ico'] ) * subItems["Cantidad"] )*subItems['Cantidad']

                    else:
                        EstadoComprobado = False
                        break
            if EstadoComprobado == False:
                break  
          




        else:

            cursor.execute("""select Articulo.IdArticulo,Articulo.Nombre,Existencia.Existencias from Articulo
            inner join Existencia on Articulo.IdArticulo = Existencia.Articulo
            where Existencia.Bodega = 13 and IdArticulo = {}""".format(itemPedido['Codigo']))

            ResultExistencias = cursor.fetchone()

            if  ResultExistencias == None:
                EstadoComprobado = False
                break
            else:
                if  int( itemPedido['Cantidad']) <=  int( ResultExistencias[2] ) :
                    
                    TotalIva =  TotalIva +  (int( itemPedido['IVA'] )/100 * itemPedido['PrecioBase'])*itemPedido["Cantidad"]
                    TotalIco = TotalIco +   (int( itemPedido['ICO'] ) * itemPedido["Cantidad"] )

                else:
                    EstadoComprobado = False
                    break

    Respuesta['TotalIva'] = TotalIva;        
    Respuesta['TotalIco'] = TotalIco; 
    Respuesta['CanFacure'] = EstadoComprobado;   



    return Respuesta  


r = requests.get( UrlApi+'/api/mobile/Prueba/GetPedidosDisponibles')

pedidosDisponibles = r.json()
PedidosCompletados = []
for pedido in pedidosDisponibles:
    
    CanFacturePedido = ComprobarExistencias(pedido['items'])

    print( CanFacturePedido )
    if CanFacturePedido["CanFacure"] == True:

        cursor.execute( QueryGetTurno() )
        Turno = int( cursor.fetchone()[0] )
        cursor.execute( QueryDocumentoInfo() )
        DocumentoInfo = cursor.fetchone()

        #print("Documento","\n",DocumentoInfo)
        
        queyconsulta = QueryExistUsuarios(pedido['Info']['NumeroIdentificacion'])   

        #print("consulta",queyconsulta)
        cursor.execute( queyconsulta )
        usuario = cursor.fetchone()
        
        
        
        if usuario == None:
                cursor.execute( QueryInsertUsuario( pedido['Info']['NumeroIdentificacion'] ,pedido['Info']['Nombres'],pedido['Info']['DireccionDomicilo'],pedido['Info']['Telefono']) )
                cnxn.commit()

        ConsultaCompleta = NonHumanFacturation(pedido,Turno,int( DocumentoInfo[1] ),int( DocumentoInfo[0] ),CanFacturePedido)  
        #print(  ConsultaCompleta,"\n \n" )
        #break
        try:
        #    #print(  ConsultaCompleta )
             cursor.execute( ConsultaCompleta )
             cnxn.commit()
             #print("Fetching one \n",cursor.fetchone())
             PedidosCompletados.append( {
                 "pedidoId":pedido['Info']['idPedido'],
                 "estado":True
             } ) 
             #PedidosCompletados.append( pedido['Info']['idPedido'] )
        except:
                         PedidosCompletados.append( {
                 "pedidoId":pedido['Info']['idPedido'],
                 "estado":False
             } )   
             print("None")



print("Pedidos completados \n",PedidosCompletados)   
payLoad = {"ArrayPedidos":json.dumps(PedidosCompletados)}

r = requests.post(UrlApi+"/api/mobile/SyncronizarFacturacionPedidos", data=payLoad)

print("Pedidos Completados","\n",r)