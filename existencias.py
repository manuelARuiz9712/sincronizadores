import pyodbc
import requests
import datetime
import json

server = 'pc\sql2008' 
database = 'ZeusPosTBS'
username = 'rappiuser' 
password = 'Key@Rappi_' 
cnxn = pyodbc.connect('DRIVER={SQL Server Native Client 10.0};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
print("Connected")
cursor = cnxn.cursor()

UrlApi = "https://pitagoras.dismelltda.com/apitest/public"



req = requests.get(UrlApi+"/api/mobile/syncronizacionProductosDisponibles")

resultado =req.json()

productosDisponibles = resultado['value']


def ConsultarIva(IdArticulo):

    Query = " select Articulo.PorcentajeIva,Articulo.Nombre from Articulo where Articulo.IdArticulo = '{}'".format(IdArticulo)
    cursor.execute(Query)
    result = cursor.fetchone()

    if result != None:
        return str( result[0]   )
    else:
        return 0   

def ConsultarExistencias(IdArticulo):

    Query = " select Existencia.Existencias from Existencia where Existencia.Articulo = '{}' and Bodega = 13 ".format(IdArticulo)
    cursor.execute(Query)
    result = cursor.fetchone()

    if result != None:
        return str( result[0] )
    else:
        return 0  

def ConsultarPreciosVentas(IdArticulo):

    Query = "   select PreciosVentas.Valor from PreciosVentas where PreciosVentas.Articulo = '{}' and TipoCliente = '173' ".format(IdArticulo)
    cursor.execute(Query)
    result = cursor.fetchone()

    if result != None:
        return str(result[0])
    else:
        return 0    

def ConsultarIco(IdArticulo):

    Query = " select GrupoDeBodegas_ImpuestoXArt.Porcentaje from GrupoDeBodegas_ImpuestoXArt where IdArticulo = '{}' and GrupoBodegaIden = 1".format(IdArticulo)
    cursor.execute(Query)
    result = cursor.fetchone()

    if result != None:
        return str( result[0] )
    else:
        return 0    





ArrayResult = []
for producto in  productosDisponibles:

#     Query ="""select Articulo.IdArticulo,Articulo.Nombre,Existencia.Existencias,Articulo.PorcentajeIva,Ex_PreciosVentas.Valor
# ,GrupoDeBodegas_ImpuestoXArt.Porcentaje as ICO 
# from Articulo
# inner join Existencia on Articulo.IdArticulo = Existencia.Articulo
# inner join Ex_PreciosVentas on Ex_PreciosVentas.Articulo = Articulo.IdArticulo
# inner join GrupoDeBodegas_ImpuestoXArt on GrupoDeBodegas_ImpuestoXArt.IdArticulo = Articulo.IdArticulo
# where Existencia.Bodega = 13 
# and Articulo.IdArticulo = {}
# and  GrupoDeBodegas_ImpuestoXArt.GrupoBodegaIden = 1
# and Ex_PreciosVentas.TipoCliente = '173' """.format( producto['Codigo'])
#     cursor.execute(Query)
#     result = cursor.fetchone()
#     print(producto['Codigo'],result,"\n")



        ArrayResult.append({  
        "productoId":producto['idProductos'],
        "existencias":ConsultarExistencias( producto['Codigo']),
         "Iva":ConsultarIva( producto['Codigo'] ),
        "Valor":ConsultarPreciosVentas( producto['Codigo'] ),
        "Ico":ConsultarIco( producto['Codigo']  )
        })

       
    

req = requests.post(UrlApi+"/api/mobile/SyncronizarInventario",{
    "productosData":json.dumps(ArrayResult)
})
#print( json.dumps(ArrayResult[0:10]) )
print( req.text )
